module dreads.kafkagroup;

// Kafka consumer-group coordinator (classic protocol, KIP-848 out of scope).
//
// OWNERSHIP: a group's state lives in TLS on the shard that owns
// keyToSlot("kafka.cg.<group>") — the same slot as its committed-offsets hash,
// so membership and offsets are co-owned. Every state transition executes in
// the owner's drain (which never yields), so each op is atomic by
// construction: no locks, no broadcast.
//
// WAIT MODEL: the coordinator never parks. Ops that must "hold" a response
// (JoinGroup barrier, follower SyncGroup) answer KG_WAIT; the CLIENT fiber
// (on its connection's shard) polls with a short sleep, bounded by the
// rebalance timeout. Per-connection head-of-line blocking during a join is
// exactly what a real broker does (in-order processing per connection).
//
// Transport: server.d routes kgroup requests via ShardMsg.kafkaGroup
// ([u64 pend][request]) and calls kgroupApply() in the owner's drain; the
// same-shard case calls it directly. See KAFKA-GROUPS-PLAN.md.

import core.time : MonoTime;

import dreads.mem : ByteBuffer;

// Kafka error codes surfaced by the FSM (wire codes, except KG_WAIT).
public enum short KG_NONE = 0;
public enum short KG_ILLEGAL_GENERATION = 22;
public enum short KG_INCONSISTENT_PROTOCOL = 23;
public enum short KG_UNKNOWN_MEMBER = 25;
public enum short KG_REBALANCE_IN_PROGRESS = 27;
public enum short KG_MEMBER_ID_REQUIRED = 79;
/// Internal sentinel: not a wire code — the caller sleeps and re-polls.
public enum short KG_WAIT = -100;

// Request opcodes ([u8 op][u16 gLen][group][...]; all ints big-endian).
public enum ubyte KGOP_JOIN = 1; // fresh JoinGroup (may trigger a rebalance)
public enum ubyte KGOP_JOIN_POLL = 2; // barrier poll (never triggers)
public enum ubyte KGOP_SYNC = 3; // SyncGroup; leader carries assignments
public enum ubyte KGOP_HEARTBEAT = 4;
public enum ubyte KGOP_LEAVE = 5;
public enum ubyte KGOP_DESCRIBE = 6;
public enum ubyte KGOP_COMMIT_CHECK = 7; // OffsetCommit generation fencing
public enum ubyte KGOP_DROP = 8; // DeleteGroups: drop an EMPTY group's state
public enum ubyte KGOP_SUBSCRIBED = 9; // OffsetDelete: is any member subscribed?
// Transaction-coordinator ops (KAFKA-TXN-PLAN.md T2) — same transport, the
// "group" field carries the transactional.id, state in a separate TLS AA.
public enum ubyte KGOP_TXN_INIT = 10; // -> [i16 err][i64 pid][i16 epoch]
public enum ubyte KGOP_TXN_ADD = 11; // register partitions in the open txn
public enum ubyte KGOP_TXN_END = 12; // -> the txn's partitions (and clears them)
public enum ubyte KGOP_TXN_OFFSETS = 13; // buffer TxnOffsetCommit until EndTxn

private enum ubyte ST_EMPTY = 0, ST_PREPARING = 1, ST_COMPLETING = 2, ST_STABLE = 3;

private enum long REBALANCE_CAP_MS = 60_000; // barrier deadline ceiling
private enum size_t KG_MAX_MEMBERS = 512; // sanity cap per group
private enum size_t KG_MAX_GROUPS = 4096; // sanity cap per shard

private struct KgMember
{
    string gii; // group.instance.id; "" = dynamic member
    string clientId; // the joining request's header client.id (DescribeGroups)
    string protoType;
    string[] protoNames; // supported protocols, preference order
    immutable(ubyte)[][] protoMetas; // metadata per protocol (same order)
    int sessMs = 45_000;
    int rebMs = 60_000;
    long lastMs; // MonoTime ms of the member's last op (heartbeat surrogate)
    bool joinedRound; // re-joined during the current rebalance round
    immutable(ubyte)[] assignment;
    bool hasAssignment;
}

private struct KgGroup
{
    ubyte state = ST_EMPTY;
    int generation = 0;
    string leader;
    string protoName;
    string protoType;
    long deadlineMs; // PreparingRebalance barrier deadline
    string[] order; // member ids in join order (leader pick + stable listing)
    KgMember[string] members;
}

private KgGroup*[string] tGroups; // TLS: groups owned by THIS shard

private struct KgTxnOff
{
    string topic;
    int part;
    long off;
    bool hasMeta;
    string meta;
}

private struct KgTxn
{
    long pid;
    short epoch;
    string[] tps; // "topic\x1fpartition" touched by the OPEN transaction
    string offGroup; // consumer group of the buffered TxnOffsetCommit
    KgTxnOff[] offs; // offsets applied on COMMIT, dropped on abort
    long lastMs; // MonoTime ms of the last txn op — idle eviction in kgroupSweep
}

private KgTxn*[string] tTxns; // TLS: transactional ids owned by THIS shard
private shared long gKgPidCtr = 5000; // producer-id source for transactional ids
private enum int TXN_MAX_TIMEOUT_MS = 900_000; // transaction.max.timeout.ms

/// Monotonic milliseconds (never the per-command frozen wall clock).
public long kgNowMs() @nogc nothrow @trusted
{
    auto t = MonoTime.currTime;
    return t.ticks / (MonoTime.ticksPerSecond / 1000);
}

// --- little wire helpers over the op payloads --------------------------------

private struct KgRd
{
    const(ubyte)[] p;
    size_t i;
    bool ok = true;

    ubyte u8() nothrow @nogc
    {
        if (i + 1 > p.length)
        {
            ok = false;
            return 0;
        }
        return p[i++];
    }

    int i32() nothrow @nogc
    {
        if (i + 4 > p.length)
        {
            ok = false;
            return 0;
        }
        immutable v = (cast(int) p[i] << 24) | (cast(int) p[i + 1] << 16)
            | (cast(int) p[i + 2] << 8) | p[i + 3];
        i += 4;
        return v;
    }

    const(char)[] str16() nothrow @nogc
    {
        if (i + 2 > p.length)
        {
            ok = false;
            return null;
        }
        immutable n = (cast(size_t) p[i] << 8) | p[i + 1];
        i += 2;
        if (i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto s = cast(const(char)[]) p[i .. i + n];
        i += n;
        return s;
    }

    const(ubyte)[] bytes32() nothrow @nogc
    {
        immutable n = cast(uint) i32();
        if (!ok || i + n > p.length)
        {
            ok = false;
            return null;
        }
        auto b = p[i .. i + n];
        i += n;
        return b;
    }
}

private void wU8(ref ByteBuffer o, ubyte v) nothrow @nogc
{
    o.appendByte(cast(char) v);
}

private void wI16(ref ByteBuffer o, short v) nothrow @nogc
{
    o.appendByte(cast(char)(cast(ushort) v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void wI32(ref ByteBuffer o, int v) nothrow @nogc
{
    o.appendByte(cast(char)(cast(uint) v >> 24));
    o.appendByte(cast(char)(cast(uint) v >> 16));
    o.appendByte(cast(char)(cast(uint) v >> 8));
    o.appendByte(cast(char)(v & 0xFF));
}

private void wStr16(ref ByteBuffer o, scope const(char)[] s) nothrow
{
    immutable n = s.length > 0xFFFF ? 0xFFFF : s.length;
    wI16(o, cast(short) n);
    o.append(s[0 .. n]);
}

private void wBytes32(ref ByteBuffer o, scope const(ubyte)[] b) nothrow
{
    wI32(o, cast(int) b.length);
    o.append(cast(const(char)[]) b);
}

// --- FSM ---------------------------------------------------------------------

private void enterPreparing(KgGroup* g, long now) nothrow
{
    g.state = ST_PREPARING;
    long maxReb = 5000;
    foreach (ref m; g.members)
    {
        if (m.rebMs > maxReb)
            maxReb = m.rebMs;
        m.joinedRound = false;
    }
    if (maxReb > REBALANCE_CAP_MS)
        maxReb = REBALANCE_CAP_MS;
    g.deadlineMs = now + maxReb;
}

/// Close the join barrier: bump generation, elect the leader, select the
/// protocol every member supports. Returns false when no common protocol
/// exists (the group resets to Empty; joiners see KG_INCONSISTENT_PROTOCOL).
private bool closeBarrier(KgGroup* g) nothrow
{
    // drop members that missed the round (deadline path); joined ones stay
    string[] keep;
    foreach (id; g.order)
        if (auto m = id in g.members)
        {
            if (m.joinedRound)
                keep ~= id;
            else
                g.members.remove(id);
        }
    g.order = keep;
    if (g.order.length == 0)
    {
        g.state = ST_EMPTY;
        return true;
    }
    // leader: previous leader when it re-joined, else first in join order
    bool leaderAlive = false;
    foreach (id; g.order)
        if (id == g.leader)
        {
            leaderAlive = true;
            break;
        }
    if (!leaderAlive)
        g.leader = g.order[0];
    // protocol: leader's preference order, first supported by all members
    auto lead = g.leader in g.members;
    g.protoName = null;
    if (lead !is null)
        outer: foreach (cand; lead.protoNames)
        {
            foreach (id; g.order)
            {
                auto m = id in g.members;
                bool has = false;
                foreach (pn; m.protoNames)
                    if (pn == cand)
                    {
                        has = true;
                        break;
                    }
                if (!has)
                    continue outer;
            }
            g.protoName = cand;
            break;
        }
    if (g.protoName is null)
    {
        // no common protocol: reset — every parked joiner answers 23
        g.state = ST_EMPTY;
        g.generation++;
        return false;
    }
    g.protoType = lead.protoType;
    g.generation++;
    g.state = ST_COMPLETING;
    foreach (ref m; g.members)
    {
        m.hasAssignment = false;
        m.assignment = null;
    }
    return true;
}

/// The member's selected-protocol metadata (for the leader's member list).
private immutable(ubyte)[] memberMeta(const KgMember* m, scope const(char)[] proto) nothrow
{
    foreach (k, pn; m.protoNames)
        if (pn == proto && k < m.protoMetas.length)
            return m.protoMetas[k];
    return m.protoMetas.length ? m.protoMetas[0] : null;
}

/// Emit the join-ok payload for `mid`:
/// [i16 0][str mid][i32 gen][u8 isLeader][str proto][str protoType][str leader]
/// [i32 nmembers]{[str mid][u8 giiNull][str gii][bytes32 meta]}* (leader only)
private void emitJoinOk(KgGroup* g, scope const(char)[] mid, ref ByteBuffer o) nothrow
{
    immutable isLeader = mid == g.leader;
    wI16(o, KG_NONE);
    wStr16(o, mid); // the id this member ended up with (assigned/reclaimed)
    wI32(o, g.generation);
    wU8(o, isLeader ? 1 : 0);
    wStr16(o, g.protoName);
    wStr16(o, g.protoType);
    wStr16(o, g.leader);
    if (!isLeader)
    {
        wI32(o, 0);
        return;
    }
    wI32(o, cast(int) g.order.length);
    foreach (id; g.order)
    {
        auto m = id in g.members;
        wStr16(o, id);
        wU8(o, m.gii.length ? 0 : 1);
        wStr16(o, m.gii);
        wBytes32(o, memberMeta(m, g.protoName));
    }
}

private shared int gKgMemberCtr; // broker-assigned member-id counter

/// Execute ONE group op atomically on the owner shard. `req` is the op
/// payload (pend already stripped); the reply layout is op-specific and
/// always starts with [i16 err] (KG_WAIT = poll again).
public void kgroupApply(scope const(ubyte)[] req, ref ByteBuffer o) nothrow @trusted
{
    o.clear();
    KgRd r = KgRd(req);
    immutable op = r.u8();
    auto groupName = r.str16();
    if (!r.ok || groupName.length == 0)
    {
        wI16(o, KG_UNKNOWN_MEMBER);
        return;
    }
    immutable now = kgNowMs();
    auto gp = groupName in tGroups;
    KgGroup* g = gp !is null ? *gp : null;

    switch (op)
    {
    default:
        wI16(o, KG_UNKNOWN_MEMBER); // unknown op: safe terminal error
        return;

    case KGOP_JOIN:
        {
            auto mid = r.str16();
            immutable giiNull = r.u8() != 0;
            auto gii = r.str16();
            immutable sessMs = r.i32();
            immutable rebMs = r.i32();
            immutable v4plus = r.u8() != 0;
            auto ptype = r.str16();
            auto clientId = r.str16();
            immutable np = r.i32();
            string[] pnames;
            immutable(ubyte)[][] pmetas;
            foreach (_; 0 .. (np < 0 || np > 64 ? 0 : np))
            {
                if (!r.ok)
                    break;
                auto pn = r.str16();
                auto pm = r.bytes32();
                if (r.ok)
                {
                    pnames ~= pn.idup;
                    pmetas ~= cast(immutable(ubyte)[]) pm.idup;
                }
            }
            if (!r.ok || pnames.length == 0)
            {
                wI16(o, KG_INCONSISTENT_PROTOCOL);
                return;
            }
            if (g is null)
            {
                if (tGroups.length >= KG_MAX_GROUPS)
                {
                    wI16(o, KG_REBALANCE_IN_PROGRESS); // safe retryable
                    return;
                }
                g = new KgGroup;
                tGroups[groupName.idup] = g;
            }
            // static membership: a known group.instance.id reclaims its id
            string useMid = null;
            if (!giiNull && gii.length)
                foreach (id; g.order)
                    if (auto m = id in g.members)
                        if (m.gii == gii)
                        {
                            useMid = id;
                            break;
                        }
            if (useMid is null && mid.length)
                useMid = mid.idup;
            if (useMid is null)
            {
                // broker-assigned id
                import core.atomic : atomicOp;
                import core.stdc.stdio : snprintf;

                char[64] buf = void;
                immutable n = atomicOp!"+="(gKgMemberCtr, 1);
                immutable k = snprintf(buf.ptr, buf.length, "dreads-%d-%d", n,
                        cast(int)(now & 0xFFFF));
                useMid = buf[0 .. (k > 0 ? k : 0)].idup;
                if (v4plus)
                {
                    // KIP-394 dance: register as a known-but-not-joined member
                    // (the barrier waits for its immediate re-join), answer 79.
                    if (g.members.length < KG_MAX_MEMBERS)
                    {
                        KgMember nm;
                        nm.gii = giiNull ? "" : gii.idup;
                        nm.clientId = clientId.idup;
                        nm.protoType = ptype.idup;
                        nm.protoNames = pnames;
                        nm.protoMetas = pmetas;
                        nm.sessMs = sessMs > 0 ? sessMs : 45_000;
                        nm.rebMs = rebMs > 0 ? rebMs : 60_000;
                        nm.lastMs = now;
                        g.members[useMid] = nm;
                        g.order ~= useMid;
                        debug (kgroup)
                        {
                            import core.stdc.stdio : fprintf, stderr;
                            fprintf(stderr, "KG JOIN-79 %.*s member %.*s n=%d\n",
                                    cast(int) groupName.length, groupName.ptr,
                                    cast(int) useMid.length, useMid.ptr,
                                    cast(int) g.members.length);
                        }
                    }
                    wI16(o, KG_MEMBER_ID_REQUIRED);
                    wStr16(o, useMid);
                    return;
                }
            }
            auto mp = useMid in g.members;
            if (mp is null)
            {
                if (g.members.length >= KG_MAX_MEMBERS)
                {
                    wI16(o, KG_REBALANCE_IN_PROGRESS);
                    return;
                }
                g.members[useMid] = KgMember.init;
                g.order ~= useMid;
                mp = useMid in g.members;
                debug (kgroup)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    fprintf(stderr, "KG JOIN-NEW %.*s member %.*s n=%d\n",
                            cast(int) groupName.length, groupName.ptr,
                            cast(int) useMid.length, useMid.ptr,
                            cast(int) g.members.length);
                }
            }
            mp.gii = giiNull ? "" : gii.idup;
            mp.clientId = clientId.idup;
            mp.protoType = ptype.idup;
            mp.protoNames = pnames;
            mp.protoMetas = pmetas;
            mp.sessMs = sessMs > 0 ? sessMs : 45_000;
            mp.rebMs = rebMs > 0 ? rebMs : 60_000;
            mp.lastMs = now;
            // a fresh join re-opens the barrier unless one is already open
            if (g.state != ST_PREPARING)
                enterPreparing(g, now);
            mp.joinedRound = true;
            // barrier closes when every known member re-joined
            bool all = true;
            foreach (ref m; g.members)
                if (!m.joinedRound)
                {
                    all = false;
                    break;
                }
            if (all)
            {
                if (!closeBarrier(g))
                {
                    wI16(o, KG_INCONSISTENT_PROTOCOL);
                    return;
                }
                emitJoinOk(g, useMid, o);
                return;
            }
            wI16(o, KG_WAIT);
            wStr16(o, useMid); // caller polls with the (possibly assigned) id
            return;
        }

    case KGOP_JOIN_POLL:
        {
            auto mid = r.str16();
            if (g is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (g.state == ST_PREPARING)
            {
                if (now >= g.deadlineMs)
                {
                    if (!closeBarrier(g))
                    {
                        wI16(o, KG_INCONSISTENT_PROTOCOL);
                        return;
                    }
                    if (g.state == ST_EMPTY || (mid in g.members) is null)
                    {
                        wI16(o, KG_UNKNOWN_MEMBER);
                        return;
                    }
                    emitJoinOk(g, mid, o);
                    return;
                }
                wI16(o, KG_WAIT);
                wStr16(o, mid);
                return;
            }
            if ((g.state == ST_COMPLETING || g.state == ST_STABLE) && mp.joinedRound)
            {
                emitJoinOk(g, mid, o);
                return;
            }
            wI16(o, KG_WAIT);
            wStr16(o, mid);
            return;
        }

    case KGOP_SYNC:
        {
            auto mid = r.str16();
            immutable gen = r.i32();
            immutable nass = r.i32();
            if (g is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (gen != g.generation)
            {
                wI16(o, KG_ILLEGAL_GENERATION);
                return;
            }
            if (g.state == ST_PREPARING)
            {
                wI16(o, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            if (g.state == ST_COMPLETING && mid == g.leader && nass > 0)
            {
                foreach (_; 0 .. (nass > cast(int) KG_MAX_MEMBERS ? 0 : nass))
                {
                    if (!r.ok)
                        break;
                    auto tmid = r.str16();
                    auto ab = r.bytes32();
                    if (!r.ok)
                        break;
                    if (auto tm = tmid in g.members)
                    {
                        tm.assignment = cast(immutable(ubyte)[]) ab.idup;
                        tm.hasAssignment = true;
                    }
                }
                foreach (ref m; g.members)
                    if (!m.hasAssignment)
                    {
                        m.assignment = null;
                        m.hasAssignment = true; // empty assignment
                    }
                g.state = ST_STABLE;
            }
            if (g.state == ST_STABLE && mp.hasAssignment)
            {
                wI16(o, KG_NONE);
                wBytes32(o, mp.assignment);
                return;
            }
            wI16(o, KG_WAIT); // follower waiting for the leader's sync
            return;
        }

    case KGOP_HEARTBEAT:
        {
            auto mid = r.str16();
            immutable gen = r.i32();
            if (g is null || g.state == ST_EMPTY)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (gen != g.generation)
            {
                wI16(o, KG_ILLEGAL_GENERATION);
                return;
            }
            if (g.state == ST_PREPARING || g.state == ST_COMPLETING)
            {
                wI16(o, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            wI16(o, KG_NONE);
            return;
        }

    case KGOP_LEAVE:
        {
            immutable nm = r.i32();
            wI16(o, KG_NONE);
            wI32(o, nm < 0 || nm > cast(int) KG_MAX_MEMBERS ? 0 : nm);
            bool removedAny = false;
            foreach (_; 0 .. (nm < 0 || nm > cast(int) KG_MAX_MEMBERS ? 0 : nm))
            {
                if (!r.ok)
                    break;
                auto mid = r.str16();
                short perr = KG_UNKNOWN_MEMBER;
                if (g !is null && (mid in g.members) !is null)
                {
                    debug (kgroup)
                    {
                        import core.stdc.stdio : fprintf, stderr;
                        fprintf(stderr, "KG LEAVE %.*s member %.*s n=%d\n",
                                cast(int) groupName.length, groupName.ptr,
                                cast(int) mid.length, mid.ptr,
                                cast(int) g.members.length);
                    }
                    g.members.remove(mid.idup);
                    string[] keep;
                    foreach (id; g.order)
                        if (id != mid)
                            keep ~= id;
                    g.order = keep;
                    removedAny = true;
                    perr = KG_NONE;
                }
                wI16(o, perr);
            }
            if (removedAny && g !is null)
            {
                if (g.order.length == 0)
                    g.state = ST_EMPTY;
                else
                    enterPreparing(g, now); // survivors must re-join
            }
            return;
        }

    case KGOP_DESCRIBE:
        {
            wI16(o, KG_NONE);
            if (g is null)
            {
                wU8(o, ST_EMPTY);
                wI32(o, 0);
                wStr16(o, "");
                wStr16(o, "");
                wI32(o, 0);
                return;
            }
            wU8(o, g.state);
            wI32(o, g.generation);
            wStr16(o, g.protoName);
            wStr16(o, g.protoType);
            wI32(o, cast(int) g.order.length);
            foreach (id; g.order)
            {
                auto m = id in g.members;
                wStr16(o, id);
                wU8(o, m.gii.length ? 0 : 1);
                wStr16(o, m.gii);
                wStr16(o, m.clientId);
                wBytes32(o, memberMeta(m, g.protoName));
                wBytes32(o, m.assignment);
            }
            return;
        }

    case KGOP_DROP:
        {
            if (g is null)
            {
                wI16(o, 69); // GROUP_ID_NOT_FOUND
                return;
            }
            evictStale(g, now); // exact-session-timeout truth (static members)
            if (g.members.length > 0 && g.state != ST_EMPTY)
            {
                debug (kgroup)
                {
                    import core.stdc.stdio : fprintf, stderr;
                    auto fm = g.order.length ? (g.order[0] in g.members) : null;
                    fprintf(stderr,
                            "KG DROP-NONEMPTY %.*s n=%d state=%d first=%.*s age=%lld sess=%d\n",
                            cast(int) groupName.length, groupName.ptr,
                            cast(int) g.members.length, cast(int) g.state,
                            cast(int)(g.order.length ? g.order[0].length : 0),
                            g.order.length ? g.order[0].ptr : "".ptr,
                            fm !is null ? cast(long)(now - fm.lastMs) : -1L,
                            fm !is null ? fm.sessMs : -1);
                }
                wI16(o, 68); // NON_EMPTY_GROUP
                return;
            }
            tGroups.remove(groupName.idup);
            wI16(o, KG_NONE);
            return;
        }

    case KGOP_SUBSCRIBED:
        {
            // [str topic] -> [i16 0][u8 subscribed] — parses each live
            // member's consumer-protocol metadata ([i16 ver][topics arr]).
            auto topic = r.str16();
            wI16(o, KG_NONE);
            ubyte sub = 0;
            if (g !is null && g.state != ST_EMPTY)
                outer2: foreach (id; g.order)
                {
                    auto m = id in g.members;
                    if (m is null)
                        continue;
                    auto meta = memberMeta(m, g.protoName);
                    if (meta.length < 6)
                        continue;
                    size_t i2 = 2; // skip version
                    immutable nt = (cast(int) meta[i2] << 24) | (cast(int) meta[i2 + 1] << 16)
                        | (cast(int) meta[i2 + 2] << 8) | meta[i2 + 3];
                    i2 += 4;
                    foreach (_2; 0 .. (nt < 0 || nt > 4096 ? 0 : nt))
                    {
                        if (i2 + 2 > meta.length)
                            break;
                        immutable tl = (cast(size_t) meta[i2] << 8) | meta[i2 + 1];
                        i2 += 2;
                        if (i2 + tl > meta.length)
                            break;
                        if (cast(const(char)[]) meta[i2 .. i2 + tl] == topic)
                        {
                            sub = 1;
                            break outer2;
                        }
                        i2 += tl;
                    }
                }
            wU8(o, sub);
            return;
        }

    case KGOP_TXN_INIT:
        {
            immutable txnTimeout = r.i32();
            if (txnTimeout > TXN_MAX_TIMEOUT_MS)
            {
                wI16(o, 50); // INVALID_TRANSACTION_TIMEOUT (0103 misuse)
                return;
            }
            auto tp = groupName in tTxns;
            KgTxn* t = tp !is null ? *tp : null;
            if (t is null)
            {
                import core.atomic : atomicOp;

                if (tTxns.length >= KG_MAX_GROUPS)
                {
                    wI16(o, 50); // INVALID_TRANSACTION_TIMEOUT: coordinator at capacity
                    return;
                }
                t = new KgTxn;
                t.pid = atomicOp!"+="(gKgPidCtr, 1);
                t.epoch = -1;
                tTxns[groupName.idup] = t;
            }
            t.epoch++; // re-init fences the previous epoch (zombie producer)
            t.tps = null;
            t.offs = null;
            t.offGroup = null;
            t.lastMs = now;
            wI16(o, KG_NONE);
            immutable long pv = t.pid;
            foreach_reverse (k; 0 .. 8)
                o.appendByte(cast(char)((pv >> (k * 8)) & 0xFF));
            wI16(o, t.epoch);
            return;
        }

    case KGOP_TXN_ADD:
        {
            long pid = 0;
            foreach (_; 0 .. 8)
                pid = (pid << 8) | r.u8();
            immutable epoch = cast(short) r.i32();
            immutable n2 = r.i32();
            auto tp = groupName in tTxns;
            if (tp is null || (*tp).pid != pid)
            {
                wI16(o, 51); // INVALID_PRODUCER_ID_MAPPING
                return;
            }
            auto t = *tp;
            if (epoch != t.epoch)
            {
                wI16(o, 47); // INVALID_PRODUCER_EPOCH: fenced
                return;
            }
            t.lastMs = now;
            foreach (_; 0 .. (n2 < 0 || n2 > 1024 ? 0 : n2))
            {
                if (!r.ok)
                    break;
                auto topic = r.str16();
                immutable part = r.i32();
                if (!r.ok)
                    break;
                if (topic.length > 249)
                    continue; // Kafka topic max is 249; refuse to truncate+collide
                char[300] tb = void;
                size_t tl = topic.length;
                tb[0 .. tl] = topic[0 .. tl];
                tb[tl] = '\x1f';
                import core.stdc.stdio : snprintf;

                immutable pl = snprintf(tb.ptr + tl + 1, tb.length - tl - 1, "%d", part);
                auto tps = (cast(const(char)[]) tb[0 .. tl + 1 + pl]).idup;
                bool have = false;
                foreach (x; t.tps)
                    if (x == tps)
                    {
                        have = true;
                        break;
                    }
                if (!have)
                    t.tps ~= tps;
            }
            wI16(o, KG_NONE);
            return;
        }

    case KGOP_TXN_OFFSETS:
        {
            long pid = 0;
            foreach (_; 0 .. 8)
                pid = (pid << 8) | r.u8();
            immutable epoch = cast(short) r.i32();
            auto grp = r.str16();
            immutable n2 = r.i32();
            auto tp = groupName in tTxns;
            if (tp is null || (*tp).pid != pid)
            {
                wI16(o, 51); // INVALID_PRODUCER_ID_MAPPING
                return;
            }
            auto t = *tp;
            if (epoch != t.epoch)
            {
                wI16(o, 47); // fenced
                return;
            }
            t.lastMs = now;
            t.offGroup = grp.idup;
            foreach (_; 0 .. (n2 < 0 || n2 > 4096 ? 0 : n2))
            {
                if (!r.ok)
                    break;
                KgTxnOff e;
                e.topic = r.str16().idup;
                e.part = r.i32();
                e.off = 0;
                foreach (_2; 0 .. 8)
                    e.off = (e.off << 8) | r.u8();
                e.hasMeta = r.u8() != 0;
                e.meta = r.str16().idup;
                if (r.ok)
                    t.offs ~= e;
            }
            wI16(o, KG_NONE);
            return;
        }

    case KGOP_TXN_END:
        {
            long pid = 0;
            foreach (_; 0 .. 8)
                pid = (pid << 8) | r.u8();
            immutable epoch = cast(short) r.i32();
            cast(void) r.u8(); // committed flag (markers written by the caller)
            auto tp = groupName in tTxns;
            if (tp is null || (*tp).pid != pid)
            {
                wI16(o, 51); // INVALID_PRODUCER_ID_MAPPING
                return;
            }
            auto t = *tp;
            if (epoch != t.epoch)
            {
                wI16(o, 47); // fenced
                return;
            }
            t.lastMs = now;
            wI16(o, KG_NONE);
            wI32(o, cast(int) t.tps.length);
            foreach (x; t.tps)
            {
                // split "topic\x1fpart"
                size_t sep = x.length;
                foreach (k, ch; x)
                    if (ch == '\x1f')
                    {
                        sep = k;
                        break;
                    }
                wStr16(o, x[0 .. sep]);
                int part = 0;
                if (sep < x.length)
                    foreach (c; x[sep + 1 .. $])
                        if (c >= '0' && c <= '9')
                            part = part * 10 + (c - '0');
                wI32(o, part);
            }
            wStr16(o, t.offGroup);
            wI32(o, cast(int) t.offs.length);
            foreach (e; t.offs)
            {
                wStr16(o, e.topic);
                wI32(o, e.part);
                foreach_reverse (k; 0 .. 8)
                    o.appendByte(cast(char)((e.off >> (k * 8)) & 0xFF));
                wU8(o, e.hasMeta ? 1 : 0);
                wStr16(o, e.meta);
            }
            t.tps = null; // txn closed
            t.offs = null;
            t.offGroup = null;
            return;
        }

    case KGOP_COMMIT_CHECK:
        {
            auto mid = r.str16();
            immutable gen = r.i32();
            if (g is null || g.state == ST_EMPTY)
            {
                // no live group: simple consumers commit freely (legacy path)
                wI16(o, KG_NONE);
                return;
            }
            auto mp = mid in g.members;
            if (mp is null)
            {
                wI16(o, KG_UNKNOWN_MEMBER);
                return;
            }
            mp.lastMs = now;
            if (gen != g.generation)
            {
                wI16(o, KG_ILLEGAL_GENERATION);
                return;
            }
            if (g.state == ST_PREPARING)
            {
                wI16(o, KG_REBALANCE_IN_PROGRESS);
                return;
            }
            wI16(o, KG_NONE);
            return;
        }
    }
}

/// Evict members whose session timeout lapsed (no slack: a live member
/// heartbeats at a third of its session timeout, and real Kafka evicts static
/// members at EXACTLY the timeout — DeleteGroups right after it must see the
/// group empty, 0081).
private void evictStale(KgGroup* g, long now) nothrow
{
    string[] dead;
    foreach (id; g.order)
        if (auto m = id in g.members)
            if (now - m.lastMs >= cast(long) m.sessMs)
                dead ~= id; // AT the timeout, like the real broker's timer
    if (dead.length == 0)
        return;
    foreach (id; dead)
        g.members.remove(id);
    string[] keep;
    foreach (id; g.order)
    {
        bool gone = false;
        foreach (d; dead)
            if (d == id)
            {
                gone = true;
                break;
            }
        if (!gone)
            keep ~= id;
    }
    g.order = keep;
    if (g.order.length == 0)
        g.state = ST_EMPTY;
    else if (g.state == ST_STABLE || g.state == ST_COMPLETING)
        enterPreparing(g, now); // survivors re-join next heartbeat
}

/// Per-shard maintenance (piggybacks the 50ms tick): close overdue barriers,
/// evict members whose session timeout lapsed, drop empty groups.
public void kgroupSweep() nothrow @trusted
{
    if (tGroups.length == 0 && tTxns.length == 0)
        return;
    immutable now = kgNowMs();
    string[] drop;
    foreach (name, g; tGroups)
    {
        if (g.state == ST_PREPARING && now >= g.deadlineMs)
            cast(void) closeBarrier(g);
        evictStale(g, now);
        if (g.state == ST_EMPTY && g.members.length == 0)
            drop ~= name;
    }
    foreach (name; drop)
        tGroups.remove(name);
    // idle transactions: evict txns whose last op is older than the max
    // transaction timeout (bounds tTxns like KG_MAX_GROUPS bounds tGroups).
    string[] dropTxn;
    foreach (name, t; tTxns)
        if (now - t.lastMs > TXN_MAX_TIMEOUT_MS)
            dropTxn ~= name;
    foreach (name; dropTxn)
        tTxns.remove(name);
}
